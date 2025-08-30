package it.costantino.astaapp.service.impl;

import it.costantino.astaapp.domain.Giocatore;
import it.costantino.astaapp.repository.GiocatoreRepository;
import it.costantino.astaapp.service.GiocatoreService;
import it.costantino.astaapp.service.dto.GiocatoreDTO;
import it.costantino.astaapp.service.mapper.GiocatoreMapper;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Service Implementation for managing {@link it.costantino.astaapp.domain.Giocatore}.
 */
@Service
@Transactional
public class GiocatoreServiceImpl implements GiocatoreService {

    private static final Logger LOG = LoggerFactory.getLogger(GiocatoreServiceImpl.class);

    private final GiocatoreRepository giocatoreRepository;

    private final GiocatoreMapper giocatoreMapper;

    public GiocatoreServiceImpl(GiocatoreRepository giocatoreRepository, GiocatoreMapper giocatoreMapper) {
        this.giocatoreRepository = giocatoreRepository;
        this.giocatoreMapper = giocatoreMapper;
    }

    @Override
    public GiocatoreDTO save(GiocatoreDTO giocatoreDTO) {
        LOG.debug("Request to save Giocatore : {}", giocatoreDTO);
        Giocatore giocatore = giocatoreMapper.toEntity(giocatoreDTO);
        giocatore = giocatoreRepository.save(giocatore);
        return giocatoreMapper.toDto(giocatore);
    }

    @Override
    public GiocatoreDTO update(GiocatoreDTO giocatoreDTO) {
        LOG.debug("Request to update Giocatore : {}", giocatoreDTO);
        Giocatore giocatore = giocatoreMapper.toEntity(giocatoreDTO);
        giocatore = giocatoreRepository.save(giocatore);
        return giocatoreMapper.toDto(giocatore);
    }

    @Override
    public Optional<GiocatoreDTO> partialUpdate(GiocatoreDTO giocatoreDTO) {
        LOG.debug("Request to partially update Giocatore : {}", giocatoreDTO);

        return giocatoreRepository
            .findById(giocatoreDTO.getId())
            .map(existingGiocatore -> {
                giocatoreMapper.partialUpdate(existingGiocatore, giocatoreDTO);

                return existingGiocatore;
            })
            .map(giocatoreRepository::save)
            .map(giocatoreMapper::toDto);
    }

    @Override
    @Transactional(readOnly = true)
    public List<GiocatoreDTO> findAll() {
        LOG.debug("Request to get all Giocatores");
        return giocatoreRepository.findAll().stream().map(giocatoreMapper::toDto).collect(Collectors.toCollection(LinkedList::new));
    }

    @Override
    @Transactional(readOnly = true)
    public Optional<GiocatoreDTO> findOne(Long id) {
        LOG.debug("Request to get Giocatore : {}", id);
        return giocatoreRepository.findById(id).map(giocatoreMapper::toDto);
    }

    @Override
    public void delete(Long id) {
        LOG.debug("Request to delete Giocatore : {}", id);
        giocatoreRepository.deleteById(id);
    }
}
