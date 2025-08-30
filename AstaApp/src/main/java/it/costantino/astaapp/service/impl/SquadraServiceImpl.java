package it.costantino.astaapp.service.impl;

import it.costantino.astaapp.domain.Squadra;
import it.costantino.astaapp.repository.SquadraRepository;
import it.costantino.astaapp.service.SquadraService;
import it.costantino.astaapp.service.dto.SquadraDTO;
import it.costantino.astaapp.service.mapper.SquadraMapper;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Service Implementation for managing {@link it.costantino.astaapp.domain.Squadra}.
 */
@Service
@Transactional
public class SquadraServiceImpl implements SquadraService {

    private static final Logger LOG = LoggerFactory.getLogger(SquadraServiceImpl.class);

    private final SquadraRepository squadraRepository;

    private final SquadraMapper squadraMapper;

    public SquadraServiceImpl(SquadraRepository squadraRepository, SquadraMapper squadraMapper) {
        this.squadraRepository = squadraRepository;
        this.squadraMapper = squadraMapper;
    }

    @Override
    public SquadraDTO save(SquadraDTO squadraDTO) {
        LOG.debug("Request to save Squadra : {}", squadraDTO);
        Squadra squadra = squadraMapper.toEntity(squadraDTO);
        squadra = squadraRepository.save(squadra);
        return squadraMapper.toDto(squadra);
    }

    @Override
    public SquadraDTO update(SquadraDTO squadraDTO) {
        LOG.debug("Request to update Squadra : {}", squadraDTO);
        Squadra squadra = squadraMapper.toEntity(squadraDTO);
        squadra = squadraRepository.save(squadra);
        return squadraMapper.toDto(squadra);
    }

    @Override
    public Optional<SquadraDTO> partialUpdate(SquadraDTO squadraDTO) {
        LOG.debug("Request to partially update Squadra : {}", squadraDTO);

        return squadraRepository
            .findById(squadraDTO.getId())
            .map(existingSquadra -> {
                squadraMapper.partialUpdate(existingSquadra, squadraDTO);

                return existingSquadra;
            })
            .map(squadraRepository::save)
            .map(squadraMapper::toDto);
    }

    @Override
    @Transactional(readOnly = true)
    public List<SquadraDTO> findAll() {
        LOG.debug("Request to get all Squadras");
        return squadraRepository.findAll().stream().map(squadraMapper::toDto).collect(Collectors.toCollection(LinkedList::new));
    }

    @Override
    @Transactional(readOnly = true)
    public Optional<SquadraDTO> findOne(Long id) {
        LOG.debug("Request to get Squadra : {}", id);
        return squadraRepository.findById(id).map(squadraMapper::toDto);
    }

    @Override
    public void delete(Long id) {
        LOG.debug("Request to delete Squadra : {}", id);
        squadraRepository.deleteById(id);
    }
}
